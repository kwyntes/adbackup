# adbackup

uses modified version of adb (3 lines commented out) that always outputs
progress info, even its output is redirected.

### features

- [x] incremental backups
- [x] recoverable in case device disconnects / user terminates backup
- [ ] custom postprocessing hooks

### issues

- for some weird fucking reason `adb.exe` sometimes randomly decides to stop
  emitting progress information at all and i have no idea why nor what to do about
  it.
