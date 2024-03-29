import os
import subprocess
import sys
import time
from bisect import bisect_left
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from queue import Empty, Queue
from threading import Thread

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


def invoke_adb(*args, stdin=None, progress_to_stop_on_error=None, raise_adb_error=False):
    try:
        proc = subprocess.Popen([ADB_EXE, *args],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                stdin=subprocess.PIPE if stdin else None,
                                encoding='utf8',
                                universal_newlines=True)

        if stdin:
            proc.stdin.write(stdin)
            proc.stdin.close()

        queue = Queue()

        def enqueue_pipe(pipe, is_stderr):
            for line in pipe:
                queue.put((is_stderr, line))
            queue.put(None)

        Thread(target=enqueue_pipe, args=[proc.stdout, False]).start()
        Thread(target=enqueue_pipe, args=[proc.stderr, True]).start()

        stdout = ''
        stderr = ''
        # this allows a KeyboardInterrupt to be handled every .1s
        # (normally queue.get prevents that, on Windows that is at least)
        while True:
            try:
                if (v := queue.get(timeout=.1)) is None:
                    break
                is_stderr, line = v

                # filter out daemon start messages
                if is_stderr and not line.startswith('*'):
                    stderr += line
                    yield True, line.rstrip('\n')
                if not is_stderr:
                    stdout += line
                    yield False, line.rstrip('\n')
            except Empty:
                pass

        proc.stdout.close()
        proc.stderr.close()

        # FIXME: this error handling is extremely extremely shitty (prints
        # ENTIRE stdout if stderr is empty)
        # REAL FIXME: allow errors to be handled by caller. somehow.
        if proc.wait() != 0:
            if progress_to_stop_on_error:
                progress_to_stop_on_error.stop()

            err = next(iter(stderr.split('\n')[-1:]), '') or \
                next(iter(stdout.split('\n')[-1:]), '')
            err = err.lstrip('adb: error: ')

            if raise_adb_error:
                raise ADBError(err)
            else:
                con.print('[on red]FATAL[/] [r]\\[ADB][/] %s' % escape(err))
                sys.exit()

    except FileNotFoundError:
        if progress_to_stop_on_error:
            progress_to_stop_on_error.stop()

        con.print('[on red]FATAL[/] ADB executable (%s) not found' %
                  escape(ADB_EXE))
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
    for _, line in invoke_adb(
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

        if os.path.isfile(os.path.join(ename, '.android_files')) or \
           os.path.isfile(os.path.join(ename, '.partial_android_files')):
            if lastbudate is None or budate > lastbudate:
                lastbudir = ename
                lastbudate = budate

        if os.path.isfile(os.path.join(ename, '.android_files')):
            if lastlinkabledate is None or budate > lastlinkabledate:
                lastlinkabledir = ename
                lastlinkabledate = budate

if lastbudir is not None:
    recovery_mode = os.path.isfile(
        os.path.join(lastbudir, '.partial_android_files'))

    with open(os.path.join(lastbudir,
                           '.partial_android_files' if recovery_mode
                           else '.android_files'), encoding='utf8') as f:
        # list of tuples of (mtime, size, fpath)
        last_android_files = [l.split('|', 2)
                              for l in f.read().splitlines()]
        # sort so we can use binary search
        last_android_files.sort(key=lambda x: x[2])

    if recovery_mode:
        if lastlinkabledir is not None:
            with open(os.path.join(lastlinkabledir, '.android_files'), encoding='utf8') as f:
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
    if lix == len(last_android_files) or last_android_files[lix][2] != fpath:
        if not recovery_mode:
            # not in last_android_files, file is new
            to_copy.append((mtime, int(size), fpath))
        else:
            llix = bisect_left(last_linkable_android_files,
                               fpath, key=lambda x: x[2])
            if llix == len(last_linkable_android_files) or last_linkable_android_files[llix][2] != fpath:
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
                self.columns = (TextColumn("Copying new/updated files... ({task.fields[fileno]}/{task.fields[totalfiles]})"),
                                BarColumn(bar_width=None),
                                "[progress.percentage]{task.percentage:>3.1f}%",
                                "•", FileSizeColumn(),
                                "/", TotalFileSizeColumn(),
                                "•", TransferSpeedColumn(),
                                "•", TimeElapsedColumn(),
                                "•", TimeRemainingColumn())
            yield self.make_tasks_table([task])


if recovery_mode and os.path.isfile(os.path.join(budir, '.rename_index')):
    with open(os.path.join(budir, '.rename_index'), encoding='utf8') as f:
        rename_index = f.read()
else:
    rename_index = ''


transferred = []


def write_rename_index():
    if not rename_index:
        return

    with open(os.path.join(budir, '.rename_index'), 'w', encoding='utf8') as f:
        f.write(rename_index)


total_bytes_to_copy = sum(size for _, size, _ in to_copy)
total_bytes_copied = 0

meta_lookup = {afpath: (mtime, size) for mtime, size, afpath in to_copy}

adb_err_handled = False
try:
    with TransferProgress() as progress:
        overall_task = progress.add_task('', start=False, kind='overall', totalfiles=len(to_copy),
                                         total=total_bytes_to_copy, fileno=' '*len(str(len(to_copy))))

        src_dsts = []
        rename_index_map = {}
        for _, _, afpath in to_copy:
            relpath = os.path.relpath(afpath, ANDROID_PATH)
            saferelpath = sanitize_filepath(relpath, platform='auto')
            if saferelpath != relpath:
                rename_index_map[afpath] = saferelpath

            src_dsts.append(afpath)
            src_dsts.append(os.path.join(budir, saferelpath))

        fileno = 0
        cur_size = 0
        cur_file = None
        file_task = None
        for is_stderr, line in invoke_adb('pull-batch', '-a', '-I', stdin='\n'.join(src_dsts),
                                          progress_to_stop_on_error=progress, raise_adb_error=True):
            # adb doesn't immediately exit on these errors
            if is_stderr:
                err = line.lstrip('adb: error: ')
                # if this somehow happens (remote object '...' doesn't exist)
                if err.startswith("remote object '") or \
                   err.startswith("failed to stat remote object '") or \
                   err.startswith("failed to create directory '") or \
                   err.startswith("cannot create '") or \
                   err.startswith("unexpected ID_DONE") or \
                   err.startswith("failed to copy '") or \
                   err.startswith("msg.data.size too large: ") or \
                   err.startswith("decompress failed") or \
                   err.startswith("cannot write '"):
                    progress.console.print(
                        "[on red]ERROR[/] [r]\\[ADB][/] %s" % escape(err))
                    adb_err_handled = True
                else:
                    adb_err_handled = False

            elif line.startswith(_s := '[batch] pulling '):
                afpath = line[len(_s):]
                fileno += 1
                total_bytes_copied += cur_size
                mtime, cur_size = meta_lookup[afpath]

                if cur_file:
                    transferred.append(cur_file)
                    if saferelpath := rename_index_map.get(cur_file[2], None):
                        rename_index += '%s --> %s\n' % (cur_file[2], saferelpath)
                cur_file = (mtime, cur_size, afpath)

                progress.start_task(overall_task)
                progress.update(overall_task,  # run while you still can
                                fileno=str(fileno).ljust(len(str(len(to_copy)))),
                                completed=total_bytes_copied)
                if file_task:
                    progress.remove_task(file_task)
                file_task = progress.add_task('', total=cur_size,
                                              filename=afpath, kind='file')

            # will output avg transfer speed info after transfer has finished,
            # otherwise: [ nn]% <filename>
            elif line.startswith('['):
                try:  # apparently shit can go wrong here too
                    percentage = int(line[1:4].strip())
                    bytes_copied = percentage/100 * cur_size
                    progress.update(file_task, completed=bytes_copied)
                    progress.update(
                        overall_task, completed=total_bytes_copied + bytes_copied)
                except ValueError:
                    pass

            elif line.startswith(_s := 'adb: warning: '):
                progress.console.print(
                    '[on yellow]WARN[/] [r]\\[ADB][/] %s' % escape(line[len(_s):]))

except (KeyboardInterrupt, ADBError) as e:
    if isinstance(e, ADBError) and not adb_err_handled:
        if e.err:
            con.print('[on red]FATAL[/] [r]\\[ADB][/] %s' % escape(e.err))
        else:
            con.print('[on red]FATAL[/] unexpected failure (device disconnected?)')

    with open(os.path.join(budir, '.partial_android_files'), 'w', encoding='utf8') as f:
        for mtime, size, fpath in transferred:
            f.write('%s|%d|%s\n' % (mtime, size, fpath))

    write_rename_index()

    con.print('[red]%s.[/] [magenta].partial_android_files[/] written.' %
              ('Interrupted' if isinstance(e, KeyboardInterrupt) else 'Transfer incomplete'))

    sys.exit()


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

with open(os.path.join(budir, '.android_files'), 'w', encoding='utf8') as f:
    for af in android_files:
        f.write('%s\n' % af)


program_time = (time.time() - program_start_time)
program_time_fmat = ('%dh ' % (program_time / 3600) if program_time >= 3600 else '') + \
                    ('%dm ' % (program_time % 3600 / 60) if program_time >= 60 else '') + \
                    ('%ds' % (program_time % 60))
con.print('[green]All operations completed in [cyan]%s[/][/]' %
          program_time_fmat)
