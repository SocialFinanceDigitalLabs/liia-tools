FROM gitpod/workspace-full

RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
ENV PATH="$HOME/.poetry/bin:$PATH"

RUN pyenv update && pyenv install 3.9.9
RUN pyenv global 3.9.9
