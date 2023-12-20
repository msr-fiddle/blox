from __future__ import print_function
import argparse
import time
import torch
import torch.nn as nn
import torch.backends.cudnn as cudnn
import torch.nn.functional as F
import torch.optim as optim
import torchvision.datasets as dset
import torch.utils.data.distributed
import torch.distributed as dist
import sys
import numpy as np
import os
import pandas as pd
import torchvision
from torchvision import transforms

sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.join(os.path.dirname(__file__), os.path.pardir), os.path.pardir
        )
    )
)

from applications.blox_enumerator import bloxEnumerate

# Benchmark settings
parser = argparse.ArgumentParser(
    description="PyTorch DP Synthetic Benchmark",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--benchmark_epoch",
    type=int,
    default=180,
    help="number of training benchmark epochs",
)
parser.add_argument(
    "--amp-fp16",
    action="store_true",
    default=False,
    help="Enables FP16 training with Apex.",
)
parser.add_argument("--dataroot", type=str, default="~/data/", help="path to dataset")
parser.add_argument(
    "--workers", type=int, help="number of data loading workers", default=2
)
parser.add_argument("--batch-size", type=int, default=64, help="input batch size")
parser.add_argument(
    "--imageSize",
    type=int,
    default=64,
    help="the height / width of the input image to network",
)
parser.add_argument("--nz", type=int, default=100, help="size of the latent z vector")
parser.add_argument("--ngf", type=int, default=64)
parser.add_argument("--ndf", type=int, default=64)
parser.add_argument(
    "--niter", type=int, default=25, help="number of epochs to train for"
)
parser.add_argument(
    "--lr", type=float, default=0.0002, help="learning rate, default=0.0002"
)
parser.add_argument(
    "--beta1", type=float, default=0.5, help="beta1 for adam. default=0.5"
)
parser.add_argument("--cuda", action="store_true", help="enables cuda")
parser.add_argument(
    "--dry-run", action="store_true", help="check a single training cycle works"
)
parser.add_argument("--netG", default="", help="path to netG (to continue training)")
parser.add_argument("--netD", default="", help="path to netD (to continue training)")
parser.add_argument(
    "--outf", default=".", help="folder to output images and model checkpoints"
)
parser.add_argument("--manualSeed", type=int, help="manual seed")
parser.add_argument(
    "--classes",
    default="bedroom",
    help="comma separated list of classes for the lsun data set",
)
parser.add_argument("--local_rank", type=int)
parser.add_argument("--job-id", type=int, default=0, help="job-id for blox scheduler")

args = parser.parse_args()


# Training
def benchmark_dcgan(model_name, batch_size):
    cudnn.benchmark = True
    job_id = args.job_id

    world_size = int(os.environ["WORLD_SIZE"])
    rank = int(os.environ["RANK"])
    local_rank = int(os.environ["LOCAL_RANK"])
    # initialize the process group
    dist.init_process_group(backend="nccl", rank=rank, world_size=world_size)

    torch.cuda.set_device(local_rank)

    dataset = dset.FakeData(
        size=100000,
        image_size=(3, args.imageSize, args.imageSize),
        transform=transforms.ToTensor(),
    )
    nc = 3
    sampler = torch.utils.data.distributed.DistributedSampler(dataset)
    dataloader = torch.utils.data.DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=(sampler is None),
        sampler=sampler,
        num_workers=2,
        pin_memory=False,
    )

    nz = int(args.nz)
    ngf = int(args.ngf)
    ndf = int(args.ndf)

    # custom weights initialization called on netG and netD
    def weights_init(m):
        classname = m.__class__.__name__
        if classname.find("Conv") != -1:
            torch.nn.init.normal_(m.weight, 0.0, 0.02)
        elif classname.find("BatchNorm") != -1:
            torch.nn.init.normal_(m.weight, 1.0, 0.02)
            torch.nn.init.zeros_(m.bias)

    class Generator(nn.Module):
        def __init__(self):
            super(Generator, self).__init__()
            self.main = nn.Sequential(
                # input is Z, going into a convolution
                nn.ConvTranspose2d(nz, ngf * 8, 4, 1, 0, bias=False),
                nn.BatchNorm2d(ngf * 8),
                nn.ReLU(True),
                # state size. (ngf*8) x 4 x 4
                nn.ConvTranspose2d(ngf * 8, ngf * 4, 4, 2, 1, bias=False),
                nn.BatchNorm2d(ngf * 4),
                nn.ReLU(True),
                # state size. (ngf*4) x 8 x 8
                nn.ConvTranspose2d(ngf * 4, ngf * 2, 4, 2, 1, bias=False),
                nn.BatchNorm2d(ngf * 2),
                nn.ReLU(True),
                # state size. (ngf*2) x 16 x 16
                nn.ConvTranspose2d(ngf * 2, ngf, 4, 2, 1, bias=False),
                nn.BatchNorm2d(ngf),
                nn.ReLU(True),
                # state size. (ngf) x 32 x 32
                nn.ConvTranspose2d(ngf, nc, 4, 2, 1, bias=False),
                nn.Tanh()
                # state size. (nc) x 64 x 64
            )

        def forward(self, input):
            output = self.main(input)
            return output

    netG = Generator().to(local_rank)
    netG.apply(weights_init)
    if args.netG != "":
        netG.load_state_dict(torch.load(args.netG))
    torch.nn.parallel.DistributedDataParallel(netG, device_ids=[local_rank])

    class Discriminator(nn.Module):
        def __init__(self):
            super(Discriminator, self).__init__()
            self.main = nn.Sequential(
                # input is (nc) x 64 x 64
                nn.Conv2d(nc, ndf, 4, 2, 1, bias=False),
                nn.LeakyReLU(0.2, inplace=True),
                # state size. (ndf) x 32 x 32
                nn.Conv2d(ndf, ndf * 2, 4, 2, 1, bias=False),
                nn.BatchNorm2d(ndf * 2),
                nn.LeakyReLU(0.2, inplace=True),
                # state size. (ndf*2) x 16 x 16
                nn.Conv2d(ndf * 2, ndf * 4, 4, 2, 1, bias=False),
                nn.BatchNorm2d(ndf * 4),
                nn.LeakyReLU(0.2, inplace=True),
                # state size. (ndf*4) x 8 x 8
                nn.Conv2d(ndf * 4, ndf * 8, 4, 2, 1, bias=False),
                nn.BatchNorm2d(ndf * 8),
                nn.LeakyReLU(0.2, inplace=True),
                # state size. (ndf*8) x 4 x 4
                nn.Conv2d(ndf * 8, 1, 4, 1, 0, bias=False),
                nn.Sigmoid(),
            )

        def forward(self, input):
            output = self.main(input)
            return output.view(-1, 1).squeeze(1)

    netD = Discriminator().to(local_rank)
    netD.apply(weights_init)
    if args.netD != "":
        netD.load_state_dict(torch.load(args.netD))
    torch.nn.parallel.DistributedDataParallel(netD, device_ids=[local_rank])

    criterion = nn.BCELoss().to(local_rank)
    real_label = 1
    fake_label = 0

    # setup optimizer
    optimizerD = optim.Adam(netD.parameters(), lr=args.lr, betas=(args.beta1, 0.999))
    optimizerG = optim.Adam(netG.parameters(), lr=args.lr, betas=(args.beta1, 0.999))
    if args.dry_run:
        args.niter = 1

    def benchmark_step(job_id):
        iter_num = 0
        enumerator = bloxEnumerate(range(1000), args.jid)
        # Prevent total batch number < warmup+benchmark situation
        while True:
            for i, data in enumerate(dataloader, 0):
                start = time.time()
                ############################
                # (1) Update D network: maximize log(D(x)) + log(1 - D(G(z)))
                ###########################
                # train with real
                netD.zero_grad()
                real_cpu = data[0].to(local_rank)
                batch_size = real_cpu.size(0)
                label = torch.full(
                    (batch_size,), real_label, dtype=real_cpu.dtype, device=local_rank
                )

                output = netD(real_cpu)
                errD_real = criterion(output, label)
                errD_real.backward()

                # train with fake
                noise = torch.randn(batch_size, nz, 1, 1, device=local_rank)
                fake = netG(noise)
                label.fill_(fake_label)
                output = netD(fake.detach())
                errD_fake = criterion(output, label)
                errD_fake.backward()
                errD = errD_real + errD_fake
                optimizerD.step()

                ############################
                # (2) Update G network: maximize log(D(G(z)))
                ###########################
                netG.zero_grad()
                label.fill_(real_label)  # fake labels are real for generator cost
                output = netD(fake)
                errG = criterion(output, label)
                errG.backward()
                optimizerG.step()
                end = time.time()
                iter_num += 1
                print(f"iter_num: {iter_num}")
                print(f"job_id: {job_id}")
                try:
                    ictr, key = enumerator.__next__()
                except:
                    break
                # since blox internally aggregates from each trainer it will account for distributed jobs
                enumerator.push_metrics({"attained_service": (end - start)})
                enumerator.push_metrics({"per_iter_time": end - start})
                if ictr is False:
                    print("Time to exit")
                    sys.exit()
            enumerator.job_exit_notify()

    print(f"==> Training {model_name} model with {batch_size} batchsize")
    benchmark_step(job_id)


if __name__ == "__main__":
    # since this example shows a single process per GPU, the number of processes is simply replaced with the
    # number of GPUs available for training.
    model_name = "DCGAN"
    batch_size = args.batch_size
    benchmark_dcgan(model_name, batch_size)
