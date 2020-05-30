# PySparkを使ったデータクレンジング
AWS Glueを使ってETLを実行してみたくなったので、簡単なクレンジング処理をPySparkで実装してみる。  
Glueを使うためにはSparkというよりGlueのライブラリを扱える必要もあるみたいですが、そこは追々。
## Dockerfileについて
こちらのサイトを参考に、AWS Glueをローカル環境でテストするためのファイルを用意しています。iuscommunity.rpmのリダイレクトが廃止になったので参照先を変更しただけです。。。
## 環境構築
docker-compose up -d --build
## テスト実行
docker exec -it gluelocal gluepytest -vv
