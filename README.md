# Higher Education ETL testing #

This project it's a test suite that aim's to cover all the jobs, triggers and crawler defined in aws, required to realiza the ETL process

To be able to test it's required

* [Python 2.7](https://www.python.org/download/releases/2.7/)
* [VirtualEnv](https://virtualenv.pypa.io/en/stable/)

First you need to create a virtual environment from the project root, and once activated install the pip dependencies


```
    virtualenv env
    source env/bin/activate
    pip install -r requirements.txt

```

Once installed all the dependencies, you need to configure the remote AWS access using your credentials

```
    aws configure
    AWS Access Key ID [None]: <<your access key (required)>>
    AWS Secret Access Key [None]:<<your secret key(required)>>
    Default region name [None]: <<working region(optional)>>
    Default output format [None]: <<output format(optional)>>
```
