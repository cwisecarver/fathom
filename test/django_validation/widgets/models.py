from django.db import models


class Widget(models.Model):
    """A trivial model so `migrate` runs a custom app migration (DDL + a
    django_migrations insert) that fathom should auto-capture as a fleet version."""

    name = models.CharField(max_length=200)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name
