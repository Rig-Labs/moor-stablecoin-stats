# Stats

Since the envio command blocks the terminal we run it on the VM using:

```
nohup pnpm dev > /dev/null 2>&1 &
```

This creates a background process without an output log, to find the process and kill it we use:

```
ps aux | grep pnpm
```

or 

```
ps aux | grep envio
```

or

```
jobs
```

and then kill it with:

```
kill <PID>
```