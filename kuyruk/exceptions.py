class KuyrukError(Exception):
    """Base class for Kuyruk exceptions."""
    pass


class Reject(KuyrukError):
    """
    The task may raise this if it does not want to process the message.
    The message will be requeued and delivered to another worker.

    """
    pass


class Discard(KuyrukError):
    """
    The task may raise this if it does not want to process the message.
    The message will be dropped.

    """
    pass


class Timeout(KuyrukError):
    """Raised if a task exceeds it's allowed run time."""
    pass


class ResultTimeout(KuyrukError):
    """
    Raised from :func:`kuyruk.Task.send_to_queue` if ``wait_result`` is set and
    reply is not received in ``wait_result`` seconds.

    """
    pass


class RemoteException(KuyrukError):
    """
    Raised from :func:`kuyruk.Task.send_to_queue` if ``wait_result`` is set and
    exception is raised on the worker while running the task.

    """
    def __init__(self, type_, value, traceback):
        self.type = type_
        self.value = value
        self.traceback = traceback
