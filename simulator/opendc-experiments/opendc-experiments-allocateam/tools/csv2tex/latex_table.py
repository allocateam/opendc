#!/usr/bin/env python

"""A quick and dirty utility tool to convert a CSV file (exported from Google Sheets) to a LaTeX table. The primary purpose of this tool is to export the timesheets for each team member to be included in the report."""

import sys
import pandas as pd

line_template = "{} & {} & {} & {} \\\\\\hline\n"

pre_template = """\\begin{longtable}{|p{3cm}|p{3cm}|p{3cm}|p{7.5cm}|}
    TOPTOPTOP
    \\toprule\\bfseries Time Category & \\bfseries Duration & \\bfseries Date & \\bfseries Description \\\\\\midrule
"""
post_template = """
\\end{longtable}
"""

top = "\\multicolumn{4}{c}{Team member: XXX }\\\\"

filename = sys.argv[1]
base_filename = filename.split(".")[0]

df = pd.read_csv(filename)

lines = []

full_names = {
    "laura": "Laura Went",
    "misha": "Misha Rigot",
    "gilles": "Gilles Magalh\~{a}es Ribeiro",
    "andrei": "Andrei-Ioan Bolos",
    "corneliu": "Corneliu Soficu",
}

replacements = {
    "&": "\&",
    "%": "\%",
    "$": "\$",
    "#": "\#",
    "{": "\{",
    "}": "\}",
    "~": "\~",
    "^": "\^",
    "\\": "\\",
    "nan": "",
}

last_line = []

for idx, row in df.iterrows():
    items = [str(row[i]) for i in range(4)]
    #items[1] = items[1].split(":00")[0]

    for i in range(4):
        for k, v in replacements.items():
            items[i] = items[i].replace(k, v)

    if items[0] == "Total":
        last_line = items
        continue

    lines.append(
        line_template.format(
            *items
        )
    )

lines.append(
    line_template.format(
        *last_line
    )
)

name = full_names[base_filename]

top = top.replace("XXX", name)

f = open(base_filename + ".tex", "w")
f.write(pre_template.replace("TOPTOPTOP", top))
for line in lines:
    f.write(line)
f.write(post_template)
