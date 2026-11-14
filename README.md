# Scratch チャット

白を基調にした、WebSocketによるリアルタイムチャットです。メッセージ入力欄の上で5色から吹き出しの色を選び、黒文字でメッセージを送信できます。参加者数と直近100件のメッセージを表示します。

## 起動方法

```sh
npm start
```

ブラウザーで `http://localhost:8000` を開きます。別のブラウザーや端末から同じURLにアクセスすると、参加者同士でリアルタイムに会話できます。

`PORT` 環境変数で待受ポートを変更できます。
WebSocketライブラリ`ws` 8.23.0は、MITライセンス表記を含めて`vendor/ws.cjs`に単一ファイルでまとめています。外部npm依存はありません。

## GitHub Pages

フロントエンドは [https://scratch-chat.github.io/](https://scratch-chat.github.io/) で公開し、`main`へのpush時にGitHub Actionsから`public/`をデプロイします。HTML、CSS、ブラウザー側JavaScript、画像は`public/index.html`にまとめています。GitHub Pagesは静的ファイルのみを配信するため、リアルタイムチャットには別途WebSocketサーバーが必要です。接続先は`public/index.html`内の`window.CHAT_WEBSOCKET_URL`で設定します。現在はCodespacesのポート8000を接続先にしています。Codespaceが停止するとチャットも利用できなくなるため、常時利用には常時稼働するWebSocketホストが必要です。

## ロック画面

テキスト入力欄以外にフォーカスがある状態で `6` → `7` → `8` → `0` と押すと、許可されたネットワークからの操作に限り、全参加者のロック画面の表示／解除を切り替えます。

許可するグローバルIPアドレスを、サーバーの `ADMIN_ALLOWED_IP` 環境変数に設定してください。例: `ADMIN_ALLOWED_IP=203.0.113.10`。GitHub Codespacesで公開する場合はCodespaces Secretとして登録し、サーバーをその環境変数付きで再起動してください。サーバーはプロキシが付与する `X-Forwarded-For` の接続元IPを使います。Codespacesの中継が接続元IPを転送しない場合、この制限は機能しません。また、グローバルIPが変わると許可設定の更新が必要です。ロック状態はサーバーの稼働中のみ保持され、再起動すると解除されます。このロック画面は表示上の制御であり、チャットのデータやWebSocket接続へのアクセス制御ではありません。