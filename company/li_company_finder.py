from consumer import *

if __name__ == "__main__":

    with open("company\\formatter.csv", "r") as f:
        data = f.read()
    sample_data = list(data.splitlines())
    li_page = print(process(sample_data, 1))
