# Cleaning Disk Space from Podman on Windows

This guide covers how to reclaim Windows host disk space after cleaning Podman containers, images, and volumes when Podman is running through WSL2.

## Why Windows disk space is not immediately reclaimed

Podman on Windows commonly stores its Linux filesystem inside a dynamically growing WSL2 `.vhdx` virtual disk.

Deleting Podman resources frees space **inside WSL**, but the `.vhdx` file on Windows usually does not shrink automatically.

The full cleanup process is:

1. Prune unused Podman resources.
2. Trim unused filesystem blocks inside the Podman WSL machine.
3. Stop Podman and WSL.
4. Compact the `.vhdx` file from Windows.
5. Verify that Windows recovered the disk space.

---

## 1. Check Podman disk usage

From PowerShell:

```powershell
podman system df
```

For more detailed information:

```powershell
podman system df -v
```

---

## 2. Remove unused Podman resources

Remove stopped containers:

```powershell
podman container prune -f
```

Remove unused images:

```powershell
podman image prune -a -f
```

Remove unused volumes:

```powershell
podman volume prune -f
```

Remove unused networks:

```powershell
podman network prune -f
```

Or perform the cleanup in one command:

```powershell
podman system prune -a --volumes -f
```

Check usage again:

```powershell
podman system df
```

---

## 3. Trim unused blocks inside the Podman machine

Run:

```powershell
podman machine ssh sudo fstrim -av
```

This tells the virtual disk which filesystem blocks are no longer being used, making VHDX compaction more effective.

If needed, you can also run it directly through WSL:

```powershell
wsl -d podman-machine-default sudo fstrim -av
```

---

## 4. Find the Podman VHDX file

Run this from PowerShell:

```powershell
Get-ChildItem "$env:USERPROFILE\.local\share\containers\podman\machine" `
    -Recurse -Filter *.vhdx -Force |
    Sort-Object Length -Descending |
    Select-Object FullName,@{N="SizeGB";E={[math]::Round($_.Length/1GB,2)}}
```

A typical location is similar to:

```text
C:\Users\<username>\.local\share\containers\podman\machine\wsl\wsldist\podman-machine-default\ext4.vhdx
```

You can also list your WSL distributions:

```powershell
wsl -l -v
```

If necessary, locate the VHDX registered for `podman-machine-default` with:

```powershell
(Get-ChildItem -Path HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss |
    Where-Object {
        $_.GetValue("DistributionName") -eq 'podman-machine-default'
    }).GetValue("BasePath") + "\ext4.vhdx"
```

---

## 5. Stop Podman and WSL completely

Stop the Podman machine:

```powershell
podman machine stop
```

Then shut down WSL:

```powershell
wsl --shutdown
```

Verify that no WSL distribution is running:

```powershell
wsl -l -v
```

Also close applications that may still be using WSL, such as:

- Podman Desktop
- Docker Desktop
- VS Code Remote WSL sessions
- Windows Terminal tabs connected to WSL

---

## 6. Compact the VHDX

Open **PowerShell or Command Prompt as Administrator**.

Start DiskPart:

```text
diskpart
```

Select the Podman VHDX file, replacing the path with the one found earlier:

```text
select vdisk file="C:\Users\<username>\.local\share\containers\podman\machine\wsl\wsldist\podman-machine-default\ext4.vhdx"
```

Verify that the correct virtual disk is selected:

```text
detail vdisk
```

Compact it:

```text
compact vdisk
```

Exit DiskPart:

```text
exit
```

> Important: Do not use `clean`, `delete`, `format`, or other destructive DiskPart commands. Only select, inspect, and compact the VHDX.

---

## 7. Verify reclaimed Windows disk space

Check free space on drive `C:`:

```powershell
Get-PSDrive C
```

Check the size of the VHDX again:

```powershell
Get-Item "C:\Users\<username>\.local\share\containers\podman\machine\wsl\wsldist\podman-machine-default\ext4.vhdx" |
    Select-Object FullName,@{N="SizeGB";E={[math]::Round($_.Length/1GB,2)}}
```

You should now see the `.vhdx` file reduced in physical size and the corresponding space returned to the Windows host.

---

## Recommended cleanup sequence

For future cleanups, the usual sequence is:

```powershell
podman system prune -a --volumes -f
podman machine ssh sudo fstrim -av
podman machine stop
wsl --shutdown
```

Then run DiskPart as Administrator:

```text
diskpart
select vdisk file="C:\Users\<username>\.local\share\containers\podman\machine\wsl\wsldist\podman-machine-default\ext4.vhdx"
detail vdisk
compact vdisk
exit
```

Finally:

```powershell
Get-PSDrive C
```

---

## Optional: Completely recreate the Podman machine

Use this only if you do not need anything stored inside the current Podman machine.

Stop the machine:

```powershell
podman machine stop
```

Delete it:

```powershell
podman machine rm -f podman-machine-default
```

Create a fresh machine:

```powershell
podman machine init
```

Start it:

```powershell
podman machine start
```

This deletes and recreates the Podman WSL virtual machine, which also removes its existing virtual disk.

---

## Summary

Pruning Podman resources alone may not recover space on the Windows host because the WSL2 `.vhdx` file does not automatically shrink.

The important additional steps are:

```text
Podman prune
    ↓
fstrim inside WSL
    ↓
podman machine stop
    ↓
wsl --shutdown
    ↓
DiskPart -> compact vdisk
    ↓
Windows host disk space recovered
```
