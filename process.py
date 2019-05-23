# def process(filename):
#     """Removes empty lines and lines that contain only whitespace, and
#     lines with comments"""
#
#     with open(filename) as in_file:
#         out_file = open('testk2.py', 'w')
#         for line in in_file:
#             if not line.strip().startswith("#") and not line.isspace():
#                 out_file.writelines(line)
#
# process('./testk.py')

def process(filename, filename2):
    """Removes empty lines and lines that contain only whitespace, and
    lines with comments"""

    with open(filename) as f1:
        with open(filename2, 'w') as f2:
            for line in f1:
                if not line.strip().startswith("# ") and not line.isspace():
                    f2.writelines(line)

process('./rearrangement.py', './real.py')
