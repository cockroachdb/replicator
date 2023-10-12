# Docker c2c demo w/ Monitoring

Simple script to compose docker environment with:
+ running MovR application
+ source cluster :: http://localhost:8080
+ target cluster :: http://localhost:8082
+ cdc-sink :: `--bindAddr :30004 --metricsAddr :30005`
+ prometheus :: http://localhost:9090
+ grafana :: http://localhost:3000/dashboards
    + `admin/cdc-sink`

## Installation
The demo scripts exist in the github repository for `cdc-sink`.
+ `git clone git@github.com:cockroachdb/cdc-sink.git` 
  ...or...
+ `gh repo clone cockroachdb/cdc-sink`

```bash
$ git clone git@github.com:cockroachdb/cdc-sink.git

Cloning into 'cdc-sink'...
remote: Enumerating objects: 5198, done.
remote: Counting objects: 100% (714/714), done.
remote: Compressing objects: 100% (365/365), done.
remote: Total 5198 (delta 455), reused 532 (delta 341), pack-reused 4484
Receiving objects: 100% (5198/5198), 1.92 MiB | 1.71 MiB/s, done.
Resolving deltas: 100% (3556/3556), done.
```

## Run the Demo
Move into the `docker_c2c` directory:
```bash
$ cd cdc-sink/scripts/docker_c2c
```
The underling `grafana`, `prometheus` and `scripts` directories are all needed to run the `run_compose.sh` script.  

```bash
glenn ~/git/cdc-sink/scripts/docker_c2c [new_scripts] $ ls
README.md		docker-compose.yml	grafana			prometheus		run_compose.sh		scripts
```

You will need to set the `COCKROACH_DEV_LICENSE` and `COCKROACH_DEV_ORGANIZATION` environment variables.
```bash
export COCKROACH_DEV_LICENSE="_your_license_key"
export COCKROACH_DEV_ORGANIZATION="_your_organization_name"
```

Finally, run the following to compose c2c Docker Demo::
+ `./run_compose.sh`

```bash
$ ./run_compose.sh
[+] Running 21/21
 ✔ Network docker_c2c_default                        Created                                                                                     0.2s
 ✔ Volume "docker_c2c_roach_source"                  Created                                                                                     0.0s
 ✔ Volume "docker_c2c_roach_target"                  Created                                                                                     0.0s
 ✔ Volume "docker_c2c_backup"                        Created                                                                                     0.0s
 ✔ Container docker_c2c-prometheus-1                 Created                                                                                     0.1s
 ✔ Container docker_c2c-roach_target-1               Started                                                                                     0.1s
 ✔ Container docker_c2c-roach_source-1               Started                                                                                     0.1s
 ✔ Container docker_c2c-roach_source_initsleep-1     Created                                                                                     0.1s
 ✔ Container docker_c2c-grafana-1                    Created                                                                                     0.1s
 ✔ Container docker_c2c-roach_target_initsleep-1     Created                                                                                     0.1s
 ✔ Container docker_c2c-roach_source_lic1-1          Created                                                                                     0.1s
 ✔ Container docker_c2c-roach_target_initsink-1      Created                                                                                     0.1s
 ✔ Container docker_c2c-cdc-sink-1                   Created                                                                                     0.0s
 ✔ Container docker_c2c-roach_source_lic2-1          Created                                                                                     0.1s
 ✔ Container docker_c2c-roach_source_movr_init-1     Created                                                                                     0.1s
 ✔ Container docker_c2c-roach_source_enable_sinks-1  Created                                                                                     0.1s
 ✔ Container docker_c2c-roach_source_backup-1        Created                                                                                     0.0s
 ✔ Container docker_c2c-roach_target_restore-1       Created                                                                                     0.1s
 ✔ Container docker_c2c-roach_source_create_feed-1   Created                                                                                     0.0s
 ✔ Container docker_c2c-roach_final_sleep-1          Created                                                                                     0.0s
 ✔ Container docker_c2c-roach_source_movr_run-1      Created                                                                                     0.0s
 ✔ Container docker_c2c-roach_source_movr_run-1      Started                                                                                     0.0s

Resetting Grafana Admin Password....

docker exec -it 70b96fb2e75a /usr/share/grafana/bin/grafana cli --homepath /usr/share/grafana admin reset-admin-password cdc-sink
INFO [10-12|06:25:54] Starting Grafana                         logger=settings version= commit= branch= compiled=1970-01-01T00:00:00Z
INFO [10-12|06:25:54] Config loaded from                       logger=settings file=/usr/share/grafana/conf/defaults.ini
INFO [10-12|06:25:54] Config overridden from Environment variable logger=settings var="GF_PATHS_DATA=/var/lib/grafana"
INFO [10-12|06:25:54] Config overridden from Environment variable logger=settings var="GF_PATHS_LOGS=/var/log/grafana"
INFO [10-12|06:25:54] Config overridden from Environment variable logger=settings var="GF_PATHS_PLUGINS=/var/lib/grafana/plugins"
INFO [10-12|06:25:54] Config overridden from Environment variable logger=settings var="GF_PATHS_PROVISIONING=/etc/grafana/provisioning"
INFO [10-12|06:25:54] Target                                   logger=settings target=[all]
INFO [10-12|06:25:54] Path Home                                logger=settings path=/usr/share/grafana
INFO [10-12|06:25:54] Path Data                                logger=settings path=/var/lib/grafana
INFO [10-12|06:25:54] Path Logs                                logger=settings path=/var/log/grafana
INFO [10-12|06:25:54] Path Plugins                             logger=settings path=/var/lib/grafana/plugins
INFO [10-12|06:25:54] Path Provisioning                        logger=settings path=/etc/grafana/provisioning
INFO [10-12|06:25:54] App mode production                      logger=settings
INFO [10-12|06:25:54] Connecting to DB                         logger=sqlstore dbtype=sqlite3
INFO [10-12|06:25:55] Starting DB migrations                   logger=migrator
INFO [10-12|06:25:55] migrations completed                     logger=migrator performed=0 skipped=493 duration=15.991ms
INFO [10-12|06:25:55] Envelope encryption state                logger=secrets enabled=true current provider=secretKey.v1

Admin password changed successfully ✔

Grafana login is:  admin/cdc-sink
Grafana URL: http://localhost:3000/login
```

Once this has completed, the demo will be running in detached mode and all the components can be accessed:
+ source cluster :: http://localhost:8080
+ target cluster :: http://localhost:8082
+ cdc-sink :: `--bindAddr :30004 --metricsAddr :30005`
+ prometheus :: http://localhost:9090
+ grafana :: http://localhost:3000/dashboards
  + `admin/cdc-sink`

