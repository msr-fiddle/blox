#!/bin/bash

if [ "$#" -ne 4 ]; then
	echo "Usage : ./run-all-workers <data-dir> <out-dir> <>CPU threads> <numgpu>"
	exit 1
fi

DATA_DIR=$1
OUT_DIR=$2
worker=$3

#Max batch that fits
num_gpu=$4
SRC="models/GNMT/"
SCRIPTS="scripts/"
#worker=$((echo $worker | python -c "print round(float(raw_input()))" ) 2> &1)
#echo $worker | python -c "print round(float(raw_input()))"
echo " Data dir is $DATA_DIR"
echo " Out dir is $OUT_DIR"

#This is per gpu worker
#for workers in 6 4 3 2 1; do
for workers in $worker; do
	result_dir="${OUT_DIR}/gnmt_w${workers}_g${num_gpu}"
	echo "result dir is $result_dir" 
	mkdir -p $result_dir
	chmod 777 $result_dir
	echo "Now running $arch for $workers workers and $batch global batch" 
	#mpstat -P ALL 1 > cpu_util.out 2>&1 &
	#$SCRIPTS/free.sh &
	#dstat -cdnmgyr --output all-utils.csv 2>&1 &
	#$SCRIPTS/gpulog.sh &
	python3 -m torch.distributed.launch --nproc_per_node=$num_gpu $SRC/train.py --seed 2  --train-loader-workers $worker --max-iterations 100  --epoch 1 --synergy --dataset-dir $DATA_DIR > stdout_3.out 2>&1			
	#pkill -f mpstat
	#pkill -f dstat
	#pkill -f free
	#pkill -f gpulog 
	#pkill -f nvidia-smi
	#mv *.log  $result_dir/
	#mv *.csv  $result_dir/
	mv stdout_3.out $result_dir/
	
done


