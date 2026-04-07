import React from "react";

interface KPIProps {
  label: string;
  value: string | number;
  description?: string;
  trend?: "up" | "down" | "neutral";
}

const KPIStatsPower: React.FC<{ metrics: KPIProps[] }> = ({ metrics }) => {
  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
      {metrics.map((m, i) => (
        <div key={i} className="rounded-lg border border-[#cfd8ea] bg-white shadow-sm p-5">
          <div className="flex items-center justify-between">
            <h3 className="text-[10px] font-semibold uppercase tracking-[0.12em] text-[#2f4b7f]">
              {m.label}
            </h3>
            {m.trend && (
              <span
                className={`text-[12px] font-bold ${
                  m.trend === "up"
                    ? "text-[#2f5597]"
                    : m.trend === "down"
                    ? "text-[#ba3f6c]"
                    : "text-[#64748b]"
                }`}
              >
                {m.trend === "up" ? "↑" : m.trend === "down" ? "↓" : "→"}
              </span>
            )}
          </div>
          <p className="mt-2 text-3xl font-semibold tracking-tight text-[#17325f]">{m.value}</p>
          {m.description && (
            <p className="mt-1 text-[10px] uppercase tracking-[0.1em] text-[#64748b] truncate">
              {m.description}
            </p>
          )}
        </div>
      ))}
    </div>
  );
};

export default KPIStatsPower;

