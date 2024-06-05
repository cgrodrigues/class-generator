# Generate Synthetic Data

1. Clone the project to your enviroment:

```
git clone https://github.com/cgrodrigues/class-generator.git

```

2. Create a enviroment variable with the home directory of the project:

```
export PROJECT_ROOT=$(pwd)

```

3. Create Directories:

```
mkdir -p  $PROJECT_ROOT/app
mkdir -p  $PROJECT_ROOT/data

```

4.  Install **Virtual Enviroment** 

To create a virtual environment in the project root directory run the following command in the folder $PROJECT_ROOT/app:

```
cd $PROJECT_ROOT/app

virtualenv -p /usr/local/bin/python3.8 .venv

source ./.venv/bin/activate

pip install -r ../requirements.txt
```

5. Run the Generation

```
python main.py
```