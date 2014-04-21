
TAG="testscalability"

mkdir -p multitest

for EXPERIMENT in threads rprop txnlen valuesize numkeys
do
    python setup_hosts.py --color -c us-west-2 --experiment $EXPERIMENT --tag $TAG --output multitest/$EXPERIMENT
    printf "Subject: FUNZO DONEZO `date`\nExperiment $EXPERIMENT has completed, sir\n" > /tmp/msg.txt
done

printf "Subject: FUNZO DONEZO ALL `date`\nExperiment has completed, sir\n" > /tmp/msg.txt
