
# ITS4US STM CDK

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project with a virtualenv stored under the `.venv` directory.

## Setting Up the Virtual Environment

If the `.venv` directory doesn't exist (fresh clone), create it using Python 3.14 as specified in `.venv/pyvenv.cfg`:

On MacOS and Linux:

```
$ python3.14 -m venv .venv
```

On Windows:

```
% python -m venv .venv
```

Then activate the virtual environment:

On MacOS and Linux:

```
$ source .venv/bin/activate
```

On Windows:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `requirements.txt` file and rerun the `python -m pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk diff`        compare deployed stacks with current state
 * `cdk deploy --all --context stage=<stage>`      deploy all stacks to your default AWS account/region where `<stage>` could be dev, stage or prod
 * `cdk docs`        open CDK documentation

## Stack Configuration

Stacks are defined and instantiated in `app.py`. Each stack represents a group of resources for a deployment environment (dev, stage, prod).

Example from app.py:

```python
base_stack = BaseStack(
    app, 
    f"Its4us-{stage.capitalize()}-Base", 
    stage=stage, 
    env=env
)
```

This creates the "Its4us-Dev-Base" stack containing networking, databases and S3 buckets resources for the development environment. Additional stacks for staging and production can be deployed as needed by specifying the `<stage>` variable in: `cdk deploy --all --context stage=<stage>`.