import logging
import shutil
import sys
import textwrap


class IndentWrappingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # Let the standard formatter interpolate the string first.
        formatted_string = super().format(record)
        message = record.message

        # If there's a message and it sits at the end of the format string
        if message and formatted_string.endswith(message) and sys.stdout.isatty():
            prefix_length = len(formatted_string) - len(message)
            prefix = formatted_string[:prefix_length]

            terminal_width = shutil.get_terminal_size(fallback=(80, 20)).columns

            # Wrap the message text, aligning subsequent lines with the prefix.
            message = textwrap.fill(
                message,
                width=terminal_width,
                initial_indent=" " * prefix_length,
                subsequent_indent=" " * prefix_length,
                break_long_words=True,
            )
            return prefix + message[prefix_length:]

        return formatted_string
