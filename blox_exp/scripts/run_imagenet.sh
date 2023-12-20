export CUDA_VISIBLE_DEVICES=$1
python -m torch.distributed.launch \
    --master_addr=localhost \
    --master_port=$2 \
    --nproc_per_node=$3 \
    --nnodes=1 \
    --node_rank=0 \
    /global/homes/s/songbian/Megatron-Resource/blox_exp/models/imagenet_ddp.py \
    --model-name=$4 \
    --batch-size=$5 \
    --job-id=$6
