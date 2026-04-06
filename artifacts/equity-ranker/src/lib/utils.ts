import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatNumber(num: number | null | undefined, decimals = 2): string {
  if (num === null || num === undefined) return "-";
  return num.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

export function formatPercent(num: number | null | undefined, decimals = 1): string {
  if (num === null || num === undefined) return "-";
  return (num * 100).toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }) + "%";
}

export function formatCompactCurrency(num: number | null | undefined): string {
  if (num === null || num === undefined) return "-";
  
  if (num >= 1e9) {
    return "$" + (num / 1e9).toLocaleString("en-US", { maximumFractionDigits: 1 }) + "B";
  }
  if (num >= 1e6) {
    return "$" + (num / 1e6).toLocaleString("en-US", { maximumFractionDigits: 1 }) + "M";
  }
  if (num >= 1e3) {
    return "$" + (num / 1e3).toLocaleString("en-US", { maximumFractionDigits: 1 }) + "K";
  }
  return "$" + num.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

export function formatCurrency(num: number | null | undefined): string {
  if (num === null || num === undefined) return "-";
  return "$" + num.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
