// @ts-check

import js from "@eslint/js";
import { defineConfig } from "eslint/config";
import tseslint from "typescript-eslint";

export default defineConfig(
  {
    extends: [js.configs.recommended, tseslint.configs.recommended],
    basePath: "packages/orm/src",
    rules: {
      "@typescript-eslint/no-namespace": "off",
      "@typescript-eslint/no-explicit-any": "off",
    },
  },
  {
    extends: [js.configs.recommended, tseslint.configs.recommended],
    basePath: "packages/fastify-typebaox-gen/src",
    rules: {
      "@typescript-eslint/no-namespace": "off",
      "@typescript-eslint/no-explicit-any": "off",
    },
  },
);
