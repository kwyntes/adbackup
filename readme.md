# adbackup

![screenshot](screenshot.png)

[**uses a modified version of adb**](https://github.com/kwyntes/adbackup-adb).

### features

- [x] incremental backups
- [x] recoverable in case device disconnects / user terminates backup
- [ ] custom postprocessing hooks

~~possible improvement: tar small files together if that's possible somehow so
small file transfers aren't impacted by the round-trip overhead of spawning
subprocesses.~~ solved by implementing `pull-batch`.

### issues

- for some weird fucking reason `adb.exe` sometimes randomly decides to stop
  emitting progress information at all and i have no idea why nor what to do about
  it.  
  like it just does sometimes. and then it works again later. the source code
  doesn't make it any clearer either.
  bug report: https://issuetracker.google.com/issues/331682040
