-- Run this in Supabase SQL Editor to create the storage bucket

-- Create the godownload bucket
INSERT INTO storage.buckets (id, name, public)
VALUES ('godownload', 'godownload', false);

-- Set up RLS policies for the bucket

-- Allow authenticated users to upload files
CREATE POLICY "Allow authenticated uploads"
ON storage.objects FOR INSERT
TO authenticated
WITH CHECK (bucket_id = 'godownload');

-- Allow authenticated users to read their own files
CREATE POLICY "Allow authenticated reads"
ON storage.objects FOR SELECT
TO authenticated
USING (bucket_id = 'godownload');

-- Allow service role to do everything (for cleanup)
CREATE POLICY "Service role full access"
ON storage.objects FOR ALL
TO service_role
USING (bucket_id = 'godownload');

-- Optional: Create a function to auto-delete old files (runs daily)
CREATE OR REPLACE FUNCTION delete_old_export_files()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  DELETE FROM storage.objects
  WHERE bucket_id = 'godownload'
    AND created_at < NOW() - INTERVAL '24 hours';
END;
$$;

-- Schedule the cleanup (requires pg_cron extension)
-- Run this if you have pg_cron enabled:
-- SELECT cron.schedule(
--   'delete-old-godownload',
--   '0 2 * * *', -- Run at 2 AM daily
--   'SELECT delete_old_export_files();'
-- );