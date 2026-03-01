/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        // Alliance colors
        'dmk': '#E31B23',
        'admk': '#00A651',
        'tvk': '#FFD700',
        'ntk': '#F39C12',
        'bjp': '#FF9933',
        'independent': '#95A5A6',
        // UI colors
        'bg-dark': '#0F172A',
        'bg-card': '#1E293B',
        'accent': '#3B82F6',
      },
      fontFamily: {
        sans: ['var(--font-geist-sans)'],
        mono: ['var(--font-geist-mono)'],
      },
    },
  },
  plugins: [],
}
