FROM nginx:alpine

COPY web/nginx.conf /etc/nginx/conf.d/default.conf
COPY web/src/ /usr/share/nginx/html/

EXPOSE 3000

CMD ["nginx", "-g", "daemon off;"]
