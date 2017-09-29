# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 29/09/2017 3:44 PM
"""

from __future__ import division
from core import env
import os


def reverse_readline(filename, buf_size=8192):
    """a generator that returns the lines of a file in reverse order"""
    with open(filename) as fh:
        segment = None
        offset = 0
        fh.seek(0, os.SEEK_END)
        file_size = remaining_size = fh.tell()
        while remaining_size > 0:
            offset = min(file_size, offset + buf_size)
            fh.seek(file_size - offset)
            buffer = fh.read(min(remaining_size, buf_size))
            remaining_size -= buf_size
            lines = buffer.split('\n')
            # the first line of the buffer is probably not a complete line so
            # we'll save it and append it to the last line of the next buffer
            # we read
            if segment is not None:
                # if the previous chunk starts right from the beginning of line
                # do not concact the segment to the last line of new chunk
                # instead, yield the segment first
                if buffer[-1] is not '\n':
                    lines[-1] += segment
                else:
                    yield segment
            segment = lines[0]
            for index in range(len(lines) - 1, 0, -1):
                if len(lines[index]):
                    yield lines[index]
        # Don't yield None if the file was empty
        if segment is not None:
            yield segment


def converged(list_path):
    """

    :param list_path:
    :return: True if converged, False if not, None if not realised
    """
    converg = None
    end_positive = 'SOLVING FOR HEAD'.lower()
    end_neg = 'FAILED TO MEET SOLVER'.lower()
    temp = reverse_readline(list_path, 100)
    for i in temp:
        if end_positive in i.lower():
            break
        elif end_neg in i.lower():
            converg = False
            break
    return converg
