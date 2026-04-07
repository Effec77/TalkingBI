type Row = Record<string, unknown>;

const formatCell = (value: unknown): string => {
  if (value === null || value === undefined) return "-";
  if (typeof value === "number") {
    if (Number.isInteger(value)) return value.toString();
    return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
  }
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "string") return value;
  return JSON.stringify(value);
};

export default function DataTable({ rows }: { rows: Row[] }) {
  if (!rows.length) return null;
  const columns = Object.keys(rows[0] || {});

  return (
    <div className="border border-[#cfd8ea] rounded-xl overflow-hidden bg-white">
      <div className="px-4 py-2 border-b border-[#dbe4f3] text-[11px] uppercase tracking-[0.16em] text-[#2f4b7f]">
        Data Preview
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead className="bg-[#eef3fb]">
            <tr>
              {columns.map((col) => (
                <th key={col} className="text-left px-4 py-2 text-xs text-[#2f4b7f] font-semibold whitespace-nowrap">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, idx) => (
              <tr key={idx} className="border-t border-[#e8eef8]">
                {columns.map((col) => (
                  <td key={col} className="px-4 py-2 text-[#334155] whitespace-nowrap">
                    {formatCell(row[col])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
