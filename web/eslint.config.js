import tsParser from "@typescript-eslint/parser";

/**
 * RFC-006 §9.1's XSS defense, first layer: "React's default escaping with
 * `dangerouslySetInnerHTML` banned by lint".
 *
 * Deliberately one rule rather than a recommended-set config. This is a *security* gate, and a
 * broad style config would bury it in nits that reviewers learn to skim — a lint run that is
 * routinely noisy is a lint run whose one load-bearing failure gets waved through.
 *
 * Both selectors are needed. The JSX form is what a component author writes; the property form
 * catches the same escape hatch smuggled through a spread (`<div {...{ dangerouslySetInnerHTML }} />`
 * or a props object built elsewhere), which the JSX selector alone does not see.
 */
export default [
  {
    files: ["src/**/*.ts", "src/**/*.tsx", "*.ts", "*.js"],
    languageOptions: {
      parser: tsParser,
      ecmaVersion: 2022,
      sourceType: "module",
      parserOptions: { ecmaFeatures: { jsx: true } },
    },
    rules: {
      "no-restricted-syntax": [
        "error",
        {
          selector: "JSXAttribute[name.name='dangerouslySetInnerHTML']",
          message:
            "dangerouslySetInnerHTML is banned (RFC-006 §9.1). The console renders attacker-influenced data — stub bodies, recorded payloads, imposter names. Render it as text.",
        },
        {
          selector: "Property[key.name='dangerouslySetInnerHTML']",
          message:
            "dangerouslySetInnerHTML is banned (RFC-006 §9.1), including via a spread props object. Render attacker-influenced data as text.",
        },
      ],
    },
  },
];
