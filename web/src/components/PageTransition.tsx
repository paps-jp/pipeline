import { motion } from "framer-motion";
import { useLocation, useOutlet } from "react-router-dom";

/**
 * react-router の <Outlet> を framer-motion で fade+slide。
 * 親 Route の element に置く想定。
 */
export function PageTransition() {
  const location = useLocation();
  const outlet = useOutlet();
  return (
    <motion.div
      key={location.pathname}
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -6 }}
      transition={{ duration: 0.12, ease: "easeOut" }}
    >
      {outlet}
    </motion.div>
  );
}
