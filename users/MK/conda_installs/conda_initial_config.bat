call conda config --add channels mullenkamp
call conda config --prepend channels conda-forge
call conda update -y conda
call conda install -y conda-build setuptools pip anaconda-client