# Control plane experiments on the test box

Scripts to build the control plane, put it on one EC2 box (Amazon Linux 2023,
arm64, region ap-south-2), run experiments there, and bring the results back.
The box has no SSH. Everything goes through AWS SSM and S3.

## Order of use

1. `build.sh`: cross compile the three tools into `build/linux-arm64/`.
2. `push.sh`: upload the binaries to `s3://BUCKET/bin/` and the on-box scripts
   to `s3://BUCKET/scripts/`.
3. `push.sh` prints the install command. Run it with `box.sh`: it copies the
   scripts folder to `/tmp/cpv-bootstrap` and runs `install.sh` from there.
   Do this first time, and after each push. Installs into `/opt/cpv`.
4. Run an experiment script on the box, for example
   `box.sh "bash /opt/cpv/scripts/NAME.sh"`, or
   `box.sh -f ops/scripts/cp-exp/onbox/NAME.sh` for a one-off script.
   Each experiment ends with `collect.sh NAME`, which uploads the results to S3.
5. `fetch-results.sh NAME` downloads the results into `build/results/`.
   Run `fetch-results.sh` with no name to list them.

Long output from `box.sh` can be cut by SSM at 24000 characters. The full
output is in the S3 results.

## Files

- Laptop: `build.sh`, `push.sh`, `box.sh`, `fetch-results.sh`.
- On the box (`onbox/`, installed to `/opt/cpv/scripts`): `install.sh`,
  `node.sh` (start, stop, kill, freeze, wipe, status, wait-ready),
  `collect.sh` (gather logs, metrics and checks, then upload).
- `onbox/defaults.sh` holds the region, table prefix, bucket, root folder and
  laptop profile. Every script sources it. Change the defaults there only.

## Rule

Nothing in this folder deletes AWS resources. The only delete is
`node.sh wipe`, which removes a local Pebble directory on the box, and it
refuses while the node runs.
