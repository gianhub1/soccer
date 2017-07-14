## Configuration 

### Download

Run `git clone https://github.com/gianhub1/soccer`

Download Flink release 1.3.1 from [Flink Download Section](https://flink.apache.org/downloads.html)

### Config

Increase in `flink-conf.yaml` for better perfomances:

    1) taskmanager.heap.mb 
    2) taskmanager.numberOfTaskSlots

## Run 

Run `start-local.sh` in `flink-1.3.1/bin` directory and open new browser window at `localhost:8081`

Upload jar from project directory through `Submit New Job` section in Flink UI, and
submit new job with parallelism <= available task slots.

Pass input and output files path for jobs as arguments (arg[0] = input, arg[1] = output).

Pass full dataset file only to `FD` queries. To generate filtered dataset, run FormatDataset class from IDE.

## Stop

To stop Flink run `stop-local.sh` script in `flink-1.3.1/bin` directory
    
    
                                                         


