# Generated by Django 3.2 on 2022-05-05 05:55

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('movies', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='filmwork',
            name='creation_date',
            field=models.DateField(blank=True, null=True, verbose_name='creation_date'),
        ),
    ]
