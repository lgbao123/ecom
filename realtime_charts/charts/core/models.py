from django.db import models

# Create your models here.
class SaleByCardType(models.Model):
    batch_no = models.IntegerField()
    card_type = models.CharField(max_length=50)
    total_sales = models.FloatField()

class SaleByCountry(models.Model):
    batch_no = models.IntegerField()
    country = models.CharField(max_length=50)
    total_sales = models.FloatField()