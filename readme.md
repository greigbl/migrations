### 共通
1. DataRobot>ワークベンチにて「＋ユースケースを作成」
2. DataRobot>ワークベンチ>追加にて「Codespaceをアップロード」
3. ダイアログが表示されたらこちらのGithub URLをペースト: https://github.com/greigbl/migrations.git

### プロジェクト移行方法
4. 移行元のUS SaaS環境からAPIキーをこちらにて取得 https://app.datarobot.com/account/developer-tools
5. 新しくできたコードスペースにある「user.yaml」の「SOURCE_API_TOKEN」に移行元のAPIキーをペースト
6. migrate - projects.ipynbというノートブック内の指示に従って移行を実施


### ユーザ移行方法
4. DataRobot＞Workbenchにてコードスペースをアップロード
5. ダイアログが表示されたらこちらのGithub URLをペースト: https://github.com/greigbl/migrations.git
6. 移行元のUS SaaS環境からAPIキーをこちらにて取得 https://app.datarobot.com/account/developer-tools
7. 新しくできたコードスペースにある「admin.yaml」に以下の用に正しい値をペースト
  - SOURCE_API_TOKEN: <移行元のAPIキーをペースト>
  - SOURCE_ORG_ID: < DataRobot管理者にて連絡された移行元の組織ID >
  - TARGET_ORG_ID: < DataRobot管理者にて連絡された移行先の組織ID >
8. migrate - users.ipynbというノートブック内の指示に従って移行を実施