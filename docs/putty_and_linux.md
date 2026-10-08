# PuTTY, edge nodes and the Linux you need

## Why PuTTY exists

At most large companies, data jobs don't run on your laptop. They run on a shared
**Linux server** inside the company network. It might be called an *edge node*, *ETL server*,
*jump host* or *app server*. Your laptop is just a window into it.

```
 Your Windows laptop                      Company network
┌──────────────────┐   SSH (port 22,    ┌───────────────────────────┐      ┌────────┐
│  PuTTY  (terminal)│ ─── encrypted) ──► │ Linux edge node           │ ───► │ Oracle │
│  WinSCP (files)   │                    │  python framework, logs,  │      └────────┘
└──────────────────┘                    │  config files, scheduler  │ ───► AWS S3
                                         └───────────────────────────┘
```

| Tool | What it does |
|------|--------------|
| **SSH** | The protocol: an encrypted remote login to a Linux machine |
| **PuTTY** | A Windows program that opens an SSH terminal |
| **PuTTYgen** | Creates SSH keys (`.ppk` files) so you can log in without typing a password |
| **WinSCP** | Drag-and-drop file copy between your laptop and the server (also over SSH) |

Windows 10/11 also has `ssh` built in (`ssh etluser@localhost -p 2222` in PowerShell).
Banks often standardise on PuTTY anyway, so learn both.

## Connecting to the practice edge node with PuTTY

1. Download PuTTY from **putty.org** (the official site; it links to chiark.greenend.org.uk).
2. Open PuTTY. In the **Session** screen:
   - Host Name: `localhost`
   - Port: `2222`
   - Connection type: **SSH**
   - Saved Sessions: type `edge-node-dev`, then click **Save**
3. Click **Open**. The first time, PuTTY warns that it doesn't know the server's key. Click **Accept**.
   (At work, compare the fingerprint with the one your platform team publishes.)
4. `login as:` **etluser**, password **etl123**
5. Change the password right away: `passwd`

Next time, just double-click `edge-node-dev` in the saved sessions list.

**Copy and paste in PuTTY:** select text with the mouse to **copy**, and **right-click** to **paste**.
Ctrl+C does **not** copy. It interrupts the running command.

## Extra exercise: key-based login (common at banks)

1. Open **PuTTYgen**, then click **Generate** and move the mouse around.
2. Click **Save private key** to save it as `edge-node-dev.ppk`. Keep it private and never share it.
3. Copy the text in the box at the top ("Public key for pasting into OpenSSH authorized_keys").
4. In your PuTTY session on the edge node, run:
   ```bash
   mkdir -p ~/.ssh && chmod 700 ~/.ssh
   nano ~/.ssh/authorized_keys      # paste the key, then Ctrl+O, Enter, Ctrl+X
   chmod 600 ~/.ssh/authorized_keys
   ```
5. In PuTTY, go to **Connection → SSH → Auth → Credentials**, browse to the `.ppk` file,
   then go back to **Session** and **Save**. Now you log in without a password.

## Linux commands you'll use every day

| Command | What it does | Example |
|---------|--------------|---------|
| `pwd` | where am I? | `pwd` |
| `ls -lh` | list files with sizes | `ls -lh logs/` |
| `cd` | change folder | `cd ~/oracle-s3-ingestion` |
| `cat` | print a whole file | `cat config/tables/northwind_orders.yaml` |
| `less` | read a long file (q quits, / searches) | `less logs/dif_20261008T....log` |
| `tail -f` | watch a log grow live (Ctrl+C stops) | `tail -f logs/*.log` |
| `tail -n 50` | last 50 lines | `tail -n 50 logs/dif_x.log` |
| `grep` | find text in files | `grep -i error logs/*.log` |
| `nano` / `vi` | edit a file | `nano config/env/dev.yaml` |
| `cp` / `mv` / `rm` | copy / move / delete | `cp a.yaml b.yaml` |
| `mkdir -p` | make folders | `mkdir -p ~/backup` |
| `chmod` | permissions | `chmod 600 ~/.dif_env` |
| `echo $?` | exit code of the last command (0 = OK) | `bash bin/run_table.sh dev x; echo $?` |
| `ps -ef \| grep dif` | is my job still running? | |
| `kill <pid>` | stop a process | `kill 1234` |
| `nohup ... &` | keep a job running after you close PuTTY | `nohup bash bin/run_table.sh dev --all > run.out 2>&1 &` |
| `df -h` / `du -sh` | disk space / folder size | `du -sh logs/` |
| `history` | commands you ran before | `history \| grep run_table` |
| `crontab -l` | list scheduled jobs | |
| `git pull` | get the latest code | |
| `aws s3 ls` | list S3 | `aws s3 ls s3://bucket/raw/ --recursive --human-readable` |

### vi survival kit
Many servers only have `vi`:
- `i` starts typing
- `Esc` stops typing
- `:wq` + Enter saves and quits
- `:q!` + Enter quits **without** saving

## Schedulers (what runs jobs at 2 AM)

Jobs are rarely started by hand in production. A scheduler calls a wrapper script
(like `bin/run_table.sh`) and reads its **exit code**:

| Exit code | Scheduler shows | Meaning in this framework |
|-----------|-----------------|---------------------------|
| 0 | SUCCESS (green) | every table loaded and validated |
| 1 | FAILURE (red) | at least one table failed (see the log) |
| 2 | FAILURE (red) | configuration or usage error |

Linux has **cron**. Many banks use enterprise schedulers such as **Autosys** or
**Control-M**. Find out in your first week which one your team uses.
