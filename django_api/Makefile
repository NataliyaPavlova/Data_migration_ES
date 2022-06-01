help:
	@echo "Docker Compose Help"
	@echo "-----------------------"
	@echo ""
	@echo "To up containers from docker-compose.yml:"
	@echo "    make up"
	@echo ""
	@echo "To build:"
	@echo "    make build"
	@echo ""
	@echo "To do makemigrations for movies:"
	@echo "    make makemigrations"
	@echo ""
	@echo "To do migrate for movies:"
	@echo "    make migrate"
	@echo ""
	@echo "To collectstatic for django:"
	@echo "    make collectstatic"
	@echo ""
	@echo "Really, really start over:"
	@echo "    make clean"
	@echo ""

start: up migrate collectstatic

up:
	@docker-compose -f docker-compose.yml up -d

makemigrations:
	@docker-compose run backend python manage.py makemigrations movies

migrate:
	@docker-compose run backend python manage.py migrate

collectstatic:
	@docker-compose run backend python manage.py collectstatic --no-input

stop:
	@docker-compose stop

build:
	@docker-compose up --build

clean: stop
	@docker-compose rm --force
	@find . -name \*.pyc -delete
