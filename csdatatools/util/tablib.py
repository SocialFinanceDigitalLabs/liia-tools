from pathlib import Path

import tabulate

__formats = {
    '.csv': ('csv', 't'),
    '.tsv': ('tsv', 't'),
    '.yml': ('yaml', 't'),
    '.yaml': ('yaml', 't'),
    '.json': ('json', 't'),
    '.xls': ('xls', 'b'),
    '.xlsx': ('xlsx', 'b'),
}

table_formats = tabulate._table_formats


def write(data, output=None, format=None):
    if output is None or output == "" or output == "-":
        print(data.export('cli', tablefmt=format, format=format))
    else:
        output = Path(output)
        format, stream_format = __formats[output.suffix]
        with open(output, f'w{stream_format}') as f:
            f.write(data.export(format))
