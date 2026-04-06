import { Router, type IRouter } from "express";
import healthRouter from "./health";
import equityRouter from "./equity";

const router: IRouter = Router();

router.use(healthRouter);
router.use(equityRouter);

export default router;
