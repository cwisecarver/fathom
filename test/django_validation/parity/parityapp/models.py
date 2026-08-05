from django.db import models


class Order(models.Model):
    """Deliberately one field per type family Django's UDFs touch."""

    created = models.DateTimeField(null=True)
    due = models.DateField(null=True)
    at = models.TimeField(null=True)
    qty = models.IntegerField(null=True)
    price = models.FloatField(null=True)
    note = models.CharField(max_length=100, null=True)
    span = models.DurationField(null=True)

    class Meta:
        app_label = "parityapp"
