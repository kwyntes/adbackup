# adbackup

uses a modified version of adb (3 lines commented out) that always outputs
progress info, even its output is redirected.

### features

- [x] incremental backups
- [x] recoverable in case device disconnects / user terminates backup
- [ ] custom postprocessing hooks

possible improvement: tar small files together if that's possible somehow so
small file transfers aren't impacted by the round-trip overhead of spawning subprocesses.

### issues

- for some weird fucking reason `adb.exe` sometimes randomly decides to stop
  emitting progress information at all and i have no idea why nor what to do about
  it.
