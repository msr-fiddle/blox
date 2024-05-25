We run Blox inside a virtual environment. We recommend using [Anaconda](https://docs.anaconda.com/free/anaconda/install/index.html) for the same. Once you've installed Anaconda, ensure that the conda command works by running ```conda -V```, which should print the Anaconda version.

Next, we create a virtual environment called ```bloxenv```. Run the following command:

```
conda create -n bloxenv python=3.8
```

To activate the environment, run the following command:

```
conda activate bloxenv
```

Now, ```cd``` into the blox repository. You should see a ```requirements.txt``` file. We will use this file to install blox's dependencies by running the following command:

```
python -m pip install -r requirements.txt
```
