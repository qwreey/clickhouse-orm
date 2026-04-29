import type { CHBuilder } from "@qwreey-clickhouse/orm";

// #region Generator
export class CHGenerator {
  private inner: CHBuilder.BuilderFactory;
  constructor(inner: CHBuilder.BuilderFactory) {
    this.inner = inner;
  }

  public generateFastifyTypeboxEndpoint() {
    `  
      instance.get(
        "/get-users",
        {
          schema: {
            summary: "유저 목록을 얻습니다",
            description: "모든 유저 정보를 얻습니다",
            response: {
              "200": TPagination(UserModel.TUser),
            },
          },
        },
        async (req, res) => {},
      );
    `;
  }
}
// #endregion Generator
