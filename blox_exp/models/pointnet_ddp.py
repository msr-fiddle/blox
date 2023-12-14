from __future__ import print_function
import argparse
import time
import torch.nn as nn
import torch.optim as optim
import torch.utils.data.distributed
import torch.distributed as dist
import torch.backends.cudnn as cudnn
import numpy as np
import os
import sys
import pandas as pd
import torchvision
import time

sys.path.append(os.path.abspath(
    os.path.join(
        os.path.join(os.path.dirname(__file__), os.path.pardir),
        os.path.pardir)
))

from torch.nn import DataParallel
from torchvision import transforms
from workloads.lucid.pointnet.dataset import ShapeNetDataset
from workloads.lucid.pointnet.pointnet import PointNetCls, feature_transform_regularizer

# Benchmark settings
parser = argparse.ArgumentParser(
    description="PyTorch Profile pointnet", formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument("--amp-fp16", action="store_true", default=False, help="Enables FP16 training with Apex.")
parser.add_argument("--feature_transform", action="store_true", help="use feature transform")
parser.add_argument("--num_points", type=int, default=2500, help="num of points for dataset")
parser.add_argument(
    '--data_dir',
    type=str,
    default="/global/cfs/cdirs/m4207/song/shapenetcore_partanno_segmentation_benchmark_v0/",
    help='Data directory'
)
parser.add_argument('--batch-size', type=int, default=64, help='batch size')
parser.add_argument('--local_rank', type=int)

args = parser.parse_args()


def benchmark_pointnet(model_name, batch_size):
    cudnn.benchmark = True

    world_size = int(os.environ['WORLD_SIZE'])
    rank = int(os.environ['RANK'])
    local_rank = int(os.environ['LOCAL_RANK'])
    # initialize the process group
    dist.init_process_group(backend="nccl", rank=rank, world_size=world_size)

    torch.cuda.set_device(local_rank)

    # specify dataset
    # print('==> Preparing data..')
    trainset = ShapeNetDataset(root=args.data_dir, classification=True, npoints=args.num_points)
    trainsampler = torch.utils.data.distributed.DistributedSampler(trainset)
    trainloader = torch.utils.data.DataLoader(
        trainset,
        batch_size,
        shuffle=(trainsampler is None),
        sampler=trainsampler,
        num_workers=2,
        pin_memory=False
    )
    num_classes = len(trainset.classes)
    # print("classes", num_classes)

    # Model
    # print('==> Building model..')
    model = PointNetCls(k=num_classes, feature_transform=args.feature_transform)
    model = model.to(local_rank)
    torch.nn.parallel.DistributedDataParallel(
        model,
        device_ids=[local_rank]
    )

    optimizer = optim.Adam(model.parameters(), lr=0.01)

    criterion = nn.CrossEntropyLoss().to(local_rank)

    # Train
    def benchmark_step():
        iter_num = 0
        model.train()
        # Prevent total batch number < warmup+benchmark situation
        while True:
            for inputs, targets in trainloader:
                optimizer.zero_grad()
                targets = targets[:, 0]
                inputs = inputs.transpose(2, 1)
                inputs, targets = inputs.to(local_rank), targets.to(local_rank)
                pred, trans, trans_feat = model(inputs)
                loss = criterion(pred, targets)
                if args.feature_transform:
                    loss += feature_transform_regularizer(trans_feat) * 0.001
                loss.backward()
                optimizer.step()
                iter_num += 1

    print(f'==> Training {model_name} model with {batch_size} batchsize')
    benchmark_step()


if __name__ == '__main__':
    model_name = 'PointNet'
    batch_size = args.batch_size
    benchmark_pointnet(model_name, batch_size)
