import inspect
import logging
import os


class ContextFilter(logging.Filter):
    def filter(self, record):
        frame = inspect.currentframe()
        while frame:
            co_name = frame.f_code.co_name
            filename = frame.f_code.co_filename
            
            """
                Dev Note:
                    update looping to exclude entire directories instead of having to list every file
                    tried a couple things but didn't work yet. Solution is necessary for better dynamic but not urgent.
            """
            if (
                "logging" not in filename
                and "loki_logging.py" not in filename
                and "filters.py" not in filename
            ):
                break
            frame = frame.f_back

        if not frame:
            # fallback if no user frame found
            frame = inspect.currentframe().f_back.f_back

        method_name = frame.f_code.co_name
        local_vars = frame.f_locals

        is_method = "self" in local_vars or "cls" in local_vars
        class_name = None

        if "self" in local_vars:
            class_name = local_vars["self"].__class__.__name__
        elif "cls" in local_vars:
            class_name = local_vars["cls"].__name__

        tags = getattr(record, "tags", {})
        tags.update({
            "method" if is_method else "function": method_name,
            **({"class": class_name} if class_name else {})
        })
        record.tags = tags

        return True