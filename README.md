Weasel is a scheduler for many task computing applications which uses a vertical scaling technique to co-locates tasks based on
their resource characteristics. This technique identifies task’s characteristics at application runtime and dynamically adapts the number and types of co-located tasks accordingly.

Dependencies:
You need to make sure you have the following installed in you home directory:
-ZeroMq (version 14.0.1)
-psutil (2.18)
If you use a different path then change the script run_weasel.sh to reflect it.
Weasel also outputs its debugging and statistics information is logs stored in /local/user_name/

HowTOs:
- To run Weasel the script run_weasel.sh can be used. It takes as input a file with the list of host ips. The first host is used by the Global Manager while the others are used by the workers.
- To stop Weasel run the script stop_weasel.sh. This takes as input the file with the list of host ips and the path to where the logs generated during the run are stored.
- Scripts to run two example applications are in the scripts/examples/ directory. The scripts are made to run on DAS4, using MemFS as a storage system.
