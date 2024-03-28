import os
import subprocess
import sys
import time
from bisect import bisect_left
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from queue import Empty, Queue

from pathvalidate import sanitize_filepath
from rich.console import Console
from rich.markup import escape
from rich.progress import (BarColumn, FileSizeColumn, MofNCompleteColumn,
                           Progress, TextColumn, TimeElapsedColumn,
                           TimeRemainingColumn, TotalFileSizeColumn,
                           TransferSpeedColumn, track)

####################################################

# make sure this will always produce a valid dirname
DATEFORMAT = r'%d-%m-%Y, %Hh%Mm%Ss'

ANDROID_PATH = '/sdcard'

####################################################


ADB_EXE = os.path.join(sys.path[0], 'adb.exe')


con = Console()

program_start_time = time.time()


class ADBError(Exception):
    def __init__(self, err: str) -> None:
        super().__init__()
        self.err = err


def invoke_adb(*args, progress_to_stop_on_error=None, raise_adb_error=False):
    try:
        proc = subprocess.Popen([ADB_EXE, *args],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                encoding='utf8',
                                universal_newlines=True)
        stdout = ''
        for line in proc.stdout:
            # filter out daemon start messages
            if not line.startswith('*'):
                stdout += line
                yield line.rstrip('\n')
        proc.stdout.close()

        if proc.wait() != 0:
            if progress_to_stop_on_error:
                progress_to_stop_on_error.stop()

            err = proc.stderr.read() or stdout
            con.print('[on red]FATAL[/] [r]\\[ADB][/] %s' %
                      escape(err), highlight=False)
            if raise_adb_error:
                raise ADBError(err)
            else:
                sys.exit()
        proc.stderr.close()

    except FileNotFoundError:
        con.print('[on red]FATAL[/] ADB executable (%s) not found' %
                  escape(ADB_EXE), highlight=False)
        sys.exit()


with con.status('Waiting for device', spinner='clock'):
    try:
        for _ in invoke_adb('wait-for-device'):
            pass
    except KeyboardInterrupt:
        sys.exit()


with con.status('Fetching file list...'):
    # printf: <last modified epoch>|<size>|<fpath>
    # will break when filenames contain newlines but that's just ridiculous
    android_files = []
    for line in invoke_adb(
            'exec-out', rf"find -H '{ANDROID_PATH}' -type f -printf '%T@|%s|%p\n'"):
        android_files.append(line)


lastbudir = None
lastbudate = None
lastlinkabledir = None
lastlinkabledate = None
for ename in os.listdir():
    if os.path.isdir(ename):
        try:
            budate = datetime.strptime(ename, DATEFORMAT)
        except ValueError:
            # couldn't parse date, is not a backup dir
            continue
        if os.path.isfile(os.path.join(ename, '.android_files')):
            if lastlinkabledate is None or budate > lastlinkabledate:
                lastlinkabledir = ename
                lastlinkabledate = budate

            if os.path.isfile(os.path.join(ename, '.partial_android_files')):
                if lastbudate is None or budate > lastbudate:
                    lastbudir = ename
                    lastbudate = budate

if lastbudir is not None:
    recovery_mode = os.path.isfile(
        os.path.join(lastbudir, '.partial_android_files'))

    with open(os.path.join(lastbudir,
                           '.partial_android_files' if recovery_mode
                           else '.android_files')) as f:
        # list of tuples of (mtime, size, fpath)
        last_android_files = [l.split('|', 2)
                              for l in f.read().splitlines()]
        # sort so we can use binary search
        last_android_files.sort(key=lambda x: x[2])

    if recovery_mode:
        if lastlinkabledir is not None:
            with open(os.path.join(lastlinkabledir, '.android_files')) as f:
                # list of tuples of (mtime, size, fpath)
                last_linkable_android_files = [l.split('|', 2)
                                               for l in f.read().splitlines()]
                # sort so we can use binary search
                last_linkable_android_files.sort(key=lambda x: x[2])
        else:
            last_linkable_android_files = []
else:
    recovery_mode = False
    last_android_files = []


budir = datetime.strftime(datetime.now(), DATEFORMAT)

if recovery_mode:
    con.print("Recovering partial backup [bold red]%s[/] into [bold magenta]%s[/]" % (
        escape(lastbudir), escape(budir)))

    os.rename(lastbudir, budir)
else:
    con.print("Backing up into [bold magenta]%s[/]" % escape(budir))


to_copy = []
to_link = []
for af in android_files:
    mtime, size, fpath = af.split('|', 2)
    lix = bisect_left(last_android_files, fpath, key=lambda x: x[2])
    if lix == len(last_android_files):
        if not recovery_mode:
            # not in last_android_files, file is new
            to_copy.append((mtime, int(size), fpath))
        else:
            llix = bisect_left(last_linkable_android_files, fpath, key=lambda x: x[2])
            if llix == len(last_linkable_android_files):
                # not in last_linkable_android_fiiles, file actually is new
                to_copy.append((mtime, int(size), fpath))
            else:
                llmtime = last_linkable_android_files[llix][0]
                if float(mtime) > float(llmtime):
                    # file has been updated since last copy
                    to_copy.append((mtime, int(size), fpath))
                else:
                    # file up to date
                    to_link.append(fpath)
    else:
        lmtime = last_android_files[lix][0]
        if float(mtime) > float(lmtime):
            # file has been updated since last copy
            to_copy.append((mtime, int(size), fpath))
        elif not recovery_mode:
            # file up to date
            to_link.append(fpath)

            # (if in recovery mode, the file will already be present in the 
            # current directory, so we don't need to link anything)


class TransferProgress(Progress):
    def get_renderables(self):
        for task in self.tasks:
            if task.fields.get('kind') == 'file':
                self.columns = (TextColumn("[bold blue]{task.fields[filename]}"),
                                BarColumn(bar_width=None),
                                "[progress.percentage]{task.percentage:>3.1f}%",
                                "•", FileSizeColumn(),
                                "/", TotalFileSizeColumn(),
                                "•", TransferSpeedColumn(),
                                "•", TimeRemainingColumn())
            elif task.fields.get('kind') == 'overall':
                self.columns = (TextColumn("Copying new/updated files..."),
                                BarColumn(bar_width=None),
                                "[progress.percentage]{task.percentage:>3.1f}%",
                                "•", MofNCompleteColumn(),
                                "•", TimeElapsedColumn())
            yield self.make_tasks_table([task])


if recovery_mode and os.path.isfile(os.path.join(budir, '.rename_index')):
    with open(os.path.join(budir, '.rename_index')) as f:
        rename_index = f.read()
else:
    rename_index = ''

transferred = []


def write_rename_index():
    if not rename_index: return

    with open(os.path.join(budir, '.rename_index'), 'w') as f:
        f.write(rename_index)

try:
    with TransferProgress() as progress:
        overall_task = progress.add_task('', kind='overall')
        for mtime, size, afpath in progress.track(to_copy, task_id=overall_task):
            relpath = os.path.relpath(afpath, ANDROID_PATH)
            saferelpath = sanitize_filepath(relpath)
            if saferelpath != relpath:
                rename_index += '%s --> %s\n' % (afpath, saferelpath)

            localpath = os.path.join(budir, saferelpath)
            os.makedirs(os.path.dirname(localpath), exist_ok=True)

            file_task = progress.add_task(
                '', total=size, kind='file', filename=afpath)
            for line in invoke_adb('pull', afpath, localpath, progress_to_stop_on_error=progress):
                # will output avg transfer speed info after transfer has finished,
                # otherwise: [ nn]% <filename>
                if line.startswith('['):
                    percentage = int(line[1:4].strip())
                    progress.update(file_task, completed=percentage/100 * size)
            progress.remove_task(file_task)

            transferred.append((mtime, size, afpath))

except (KeyboardInterrupt, ADBError):
    with open(os.path.join(budir, '.partial_android_files'), 'w') as f:
        for mtime, size, fpath in transferred:
            f.write('%s|%d|%s\n' % (mtime, size, fpath))

    write_rename_index()


for afpath in track(to_link, description='Hardlinking previously copied files...'):
    relpath = os.path.relpath(afpath, ANDROID_PATH)
    saferelpath = sanitize_filepath(relpath)
    if saferelpath != relpath:
        rename_index += '%s --> %s\n' % (afpath, saferelpath)

    localpath = os.path.join(budir, saferelpath)
    os.makedirs(os.path.dirname(localpath), exist_ok=True)

    srcpath = os.path.join(lastlinkabledir, saferelpath)

    os.link(srcpath, localpath)

if recovery_mode:
    os.remove(os.path.join(budir, '.partial_android_files'))

with open(os.path.join(budir, '.android_files'), 'w'):
    for af in android_files:
        f.write('%s\n' % af)

program_time = (time.time() - program_start_time)
program_time_fmat = ('%dh ' % (program_time / 3600) if program_time >= 3600 else '') + \
                    ('%dm ' % (program_time % 3600 / 60) if program_time >= 60 else '') + \
                    ('%ds' % (program_time % 60))
con.print('[green]All operations completed in [cyan]%s[/][/]' % program_time_fmat)
