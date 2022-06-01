from django.contrib import admin

from .models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name','description',)
    list_filter = ('name',)
    search_fields = ('name',)

@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name',)
    list_filter = ('full_name',)
    search_fields = ('full_name',)

class PersonFilmworkInline(admin.StackedInline):
    model = PersonFilmwork

class GenreFilmworkInline(admin.StackedInline):
    model = GenreFilmwork

@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline,PersonFilmworkInline,)
    list_display = ('title','type','creation_date','rating',)
    list_filter = ('type','creation_date','rating',)
    search_fields = ('title','description','id',)


