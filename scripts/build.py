import sys
import subprocess


def linters():
    cmd = (
        "black pytransflow; "
        "poetry run python -m pylint pytransflow; "
        "poetry run python -m mypy --strict pytransflow"
    )
    subprocess.run(cmd, shell=True)


def tests():
    cmd = (
        "poetry run python -m pytest "
        "--cov-fail-under=100 "
        "--cov-report term "
        "--cov-report html:cov_html "
        "--cov=pytransflow tests/"
    )
    subprocess.run(cmd.split())


def docs():
    cmd = """
        cd docs
        make clean
        rm -rf build
        rm -rf source/api 
        sphinx-apidoc -fMe -t source/_templates/ -o source/api ../pytransflow/ 
        make html
        """
    subprocess.check_call(cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr)
