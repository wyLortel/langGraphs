# 1단계: Node로 React를 정적 파일(html/js/css)로 빌드
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json ./
RUN npm install --no-audit --no-fund
COPY . .
RUN npm run build

# 2단계: 빌드 결과만 nginx에 복사 (실행 시 Node는 필요 없다 -> 웹 서버는 '파일 배달 + /api 전달'만 함)
FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
