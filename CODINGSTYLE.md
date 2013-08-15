Python code follows PEP8 [1] with regard to coding style and PEP257 [2] with
regard to docstring style. Multi-line docstrings should have one short summary
line, followed by a blank line and a series of paragraphs. The last paragraph
should be followed by a line that closes the docstring (no blank line in
between). Here's an example:

def unlink(f):
    """Delete a file at path 'f' if it currently exists.

    Unlike os.unlink(), does not throw an exception if the file didn't already
    exist.
    """
    #code...

Module-level docstrings follow exactly the same guidelines but without the
blank line between the summary and the details.


[1]:http://www.python.org/dev/peps/pep-0008/
[2]:http://www.python.org/dev/peps/pep-0257/
