import pathlib
from pathlib import Path

file_prefix_template = '''
{
 "cells": [
'''

file_postfix_template = '''
 ],
 "metadata": {
  "kernelspec": {
   "display_name": ".venv",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.4"
  },
  "orig_nbformat": 4
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
'''

cell_prefix_template = '''
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
'''

cell_postfix_template = '''
   ]
  }
'''

code_template = '''
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "vscode": {
     "languageId": "plaintext"
    }
   },
   "outputs": [],
   "source": []
  }
'''

def create_ipynb(answer_file:Path ,index_file: Path):
    with index_file.open("r") as f:
        lines = f.readlines()
        with answer_file.open("w") as af:
            af.write(file_prefix_template)
            af.write(cell_prefix_template)
            for line in lines:
                
                if line.startswith("##"):
                    af.write(f"{cell_postfix_template},")
                    af.write(cell_prefix_template)
                    af.write(f"\"### 回答\\n\"\n")
                    af.write(f"{cell_postfix_template},")
                    af.write(f"{code_template},")
                    af.write(cell_prefix_template)
                    af.write(f"\"{''.join(line.splitlines())}\\n\"\n")
                else:
                    if not line.startswith("# "):
                        af.write(",\n")
                    af.write(f"\"{''.join(line.splitlines())}\\n\"")
            
            af.write(f"{cell_postfix_template},")
            af.write(cell_prefix_template)
            af.write(f"\"### 回答\\n\"\n")
            af.write(f"{cell_postfix_template},")
            af.write(code_template)
            af.write(file_postfix_template)
            af.flush()

def check_and_create_ipynb(target_dir: Path):
    answer_file = target_dir.joinpath("answer.ipynb")
    index_file = target_dir.joinpath("index.md")
    if answer_file.exists():
        print(f"Already exists {target_dir}/answer.ipynb file.")
    elif index_file.exists():
        create_ipynb(answer_file=answer_file, index_file=index_file)
        print(f"Create {target_dir}/answer.ipynb.")
    else:
        print(f"Skip {target_dir} because of no index.md file")


def main():
    basedir = pathlib.Path(".")
    for temp in basedir.iterdir():
        if temp.is_dir():
            if not temp.name.startswith(".") and not temp.name.startswith("esci-"):
                check_and_create_ipynb(temp)

if __name__ == "__main__":
    main()
