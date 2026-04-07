/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        primary: "#4A90D9",
        "primary-dark": "#1A1A2E",
        accent: "#7C4DFF",
        success: "#00C897",
        warning: "#F5A623",
        error: "#FF5252",
        background: "#0F0F1A",
        surface: "#1C1C2E",
        "surface-hover": "#252540",
      },
      fontFamily: {
        sans: ['Inter', 'sans-serif'],
        mono: ['JetBrains Mono', 'monospace'],
      }
    },
  },
  plugins: [],
}
