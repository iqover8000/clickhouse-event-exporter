set dotenv-load

_default:
    just --list

# this help message (default target)
help:
    just --list

# lint all .yaml / .yml files
yamllint:
    yamllint -c .yamllint .

# run python linter
flake8:
    flake8 application

# yamllint, flake8 in one target
pre-commit:
    @just yamllint
    @just flake8
