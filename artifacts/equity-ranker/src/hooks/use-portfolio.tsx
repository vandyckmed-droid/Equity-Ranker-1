import React, { createContext, useContext, useState, useEffect, ReactNode } from "react";
import { Stock } from "@workspace/api-client-react";

const BASKET_KEY = "qt:basket-v1";

function loadBasket(): string[] {
  try {
    const raw = localStorage.getItem(BASKET_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed) && parsed.every((t) => typeof t === "string")) {
      return parsed;
    }
  } catch {}
  return [];
}

function saveBasket(tickers: string[]) {
  try {
    localStorage.setItem(BASKET_KEY, JSON.stringify(tickers));
  } catch {}
}

interface PortfolioContextType {
  basket: string[];
  basketSet: Set<string>;
  addToBasket: (ticker: string) => void;
  removeFromBasket: (ticker: string) => void;
  clearBasket: () => void;
  seedBasket: (tickers: string[]) => void;
  allStocks: Stock[];
  setAllStocks: (stocks: Stock[]) => void;
  /** Current ranked universe (alpha-sorted, mcap-filtered) — used for seeding */
  rankedStocks: Stock[];
  setRankedStocks: (stocks: Stock[]) => void;
}

const PortfolioContext = createContext<PortfolioContextType | undefined>(undefined);

export function PortfolioProvider({ children }: { children: ReactNode }) {
  const [basket, setBasket] = useState<string[]>(loadBasket);
  const [allStocks, setAllStocks] = useState<Stock[]>([]);
  const [rankedStocks, setRankedStocks] = useState<Stock[]>([]);

  useEffect(() => {
    saveBasket(basket);
  }, [basket]);

  const addToBasket = (ticker: string) => {
    setBasket((prev) => (prev.includes(ticker) ? prev : [...prev, ticker]));
  };

  const removeFromBasket = (ticker: string) => {
    setBasket((prev) => prev.filter((t) => t !== ticker));
  };

  const clearBasket = () => {
    setBasket([]);
  };

  const seedBasket = (tickers: string[]) => {
    const deduped = [...new Set(tickers)];
    setBasket(deduped);
  };

  const basketSet = new Set(basket);

  return (
    <PortfolioContext.Provider
      value={{
        basket,
        basketSet,
        addToBasket,
        removeFromBasket,
        clearBasket,
        seedBasket,
        allStocks,
        setAllStocks,
        rankedStocks,
        setRankedStocks,
      }}
    >
      {children}
    </PortfolioContext.Provider>
  );
}

export function usePortfolio() {
  const context = useContext(PortfolioContext);
  if (context === undefined) {
    throw new Error("usePortfolio must be used within a PortfolioProvider");
  }
  return context;
}

/** @deprecated — kept for legacy call-sites only. Use basket/basketSet directly. */
export interface PortfolioHolding {
  ticker: string;
}
