import React, { createContext, useContext, useState, ReactNode } from "react";
import { Stock } from "@workspace/api-client-react";

export interface PortfolioHolding {
  ticker: string;
  weight: number;
}

interface PortfolioContextType {
  holdings: PortfolioHolding[];
  addHolding: (ticker: string) => void;
  removeHolding: (ticker: string) => void;
  updateWeight: (ticker: string, weight: number) => void;
  clearHoldings: () => void;
  setHoldings: (holdings: PortfolioHolding[]) => void;
  allStocks: Stock[];
  setAllStocks: (stocks: Stock[]) => void;
}

const PortfolioContext = createContext<PortfolioContextType | undefined>(undefined);

export function PortfolioProvider({ children }: { children: ReactNode }) {
  const [holdings, setHoldingsState] = useState<PortfolioHolding[]>([]);
  const [allStocks, setAllStocks] = useState<Stock[]>([]);

  const addHolding = (ticker: string) => {
    setHoldingsState((prev) => {
      if (prev.find((h) => h.ticker === ticker)) return prev;
      return [...prev, { ticker, weight: 0 }];
    });
  };

  const removeHolding = (ticker: string) => {
    setHoldingsState((prev) => prev.filter((h) => h.ticker !== ticker));
  };

  const updateWeight = (ticker: string, weight: number) => {
    setHoldingsState((prev) =>
      prev.map((h) => (h.ticker === ticker ? { ...h, weight } : h))
    );
  };

  const clearHoldings = () => {
    setHoldingsState([]);
  };

  return (
    <PortfolioContext.Provider
      value={{
        holdings,
        addHolding,
        removeHolding,
        updateWeight,
        clearHoldings,
        setHoldings: setHoldingsState,
        allStocks,
        setAllStocks,
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
