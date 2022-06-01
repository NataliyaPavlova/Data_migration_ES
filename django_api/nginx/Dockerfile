FROM nginx:1.19.2

WORKDIR /app
RUN rm /etc/nginx/conf.d/default.conf
CMD ["nginx-debug", "-g", "daemon off;"]
