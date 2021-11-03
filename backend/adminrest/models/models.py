from django.db import models


class Community(models.Model):
    name = models.CharField(max_length=255, unique=True)


class Database(models.Model):
    community = models.ForeignKey(Community, related_name="databases", on_delete=models.CASCADE)
    name = models.CharField(max_length=255, unique=True)
    unique_identifier = models.CharField(max_length=255, unique=True)


STATUS_STOPPED = "STOPPED"
STATUS_ACTIVE = "ACTIVE"
STATUS_CHOICES = (
    (STATUS_STOPPED, "Stopped"),
    (STATUS_ACTIVE, "Active"),
)


class Filter(models.Model):
    communities = models.ManyToManyField(Community, related_name="filters")
    name = models.CharField(max_length=255, unique=True)
    filter = models.TextField(null=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_CHOICES[0][0])


class FilterSelection(models.Model):
    filter = models.ForeignKey(Filter, related_name="selections", on_delete=models.CASCADE)
    column = models.CharField(max_length=255)

    def __str__(self):
        return self.column


class Application(models.Model):
    community = models.ForeignKey(Community, related_name="applications", on_delete=models.CASCADE)
    filter = models.ForeignKey(Filter, null=True, related_name="applications", on_delete=models.CASCADE)
    name = models.CharField(max_length=255, unique=True)
    request_template = models.TextField()
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_CHOICES[0][0])


class DatabaseHealthCheck(models.Model):
    database = models.ForeignKey(Database, on_delete=models.CASCADE)
    time = models.DateTimeField()
    reason = models.TextField()


class DatabaseUpload(models.Model):
    database = models.ForeignKey(Database, on_delete=models.CASCADE)
    time = models.DateTimeField()
    rows = models.IntegerField()


class ApplicationSentData(models.Model):
    application = models.ForeignKey(Application, on_delete=models.CASCADE)
    time = models.DateTimeField()
    response_code = models.IntegerField()
    response_data = models.TextField()


"""
class SubscriptionLog(models.Model):
    subscription = models.ForeignKey(Subscription, related_name="logs", on_delete=models.CASCADE)
    success_count = models.IntegerField()


class Request(models.Model):
    subscription = models.ForeignKey(Subscription, related_name="requests", on_delete=models.CASCADE)
    success_condition_template = models.TextField(null=True)
    order = models.IntegerField()

    class Meta:
        ordering = ("order",)

class RequestLog(models.Model):
    request = models.ForeignKey(Request, related_name="logs", on_delete=models.CASCADE)
    success = models.BooleanField()
    exception = models.TextField()
"""
