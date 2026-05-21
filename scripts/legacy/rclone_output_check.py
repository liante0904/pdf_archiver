import subprocess

cmd = ["rclone", "lsf", "-R", "--format", "psh", "--files-only", "onedrive:/archive/pdf/2021-01/LS증권"]
proc = subprocess.run(cmd, capture_output=True, text=True)
lines = proc.stdout.splitlines()

print("Sample rclone output:")
for line in lines[:10]:
    print(line)
